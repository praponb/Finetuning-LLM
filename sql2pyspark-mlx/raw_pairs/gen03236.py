from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("created_at"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

enriched = (
    exploded
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .filter(F.col("c.email").like("%@GMAIL.COM"))
    .select("src.*", F.col("c.city"), F.col("c.segment"))
)

enriched_carts = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.element",
        "s.city",
        F.col("b.country"),
        F.col("b.segment").alias("customer_segment"),
        F.col("b.city").alias("customer_city"),
    )
)

joined = (
    enriched_carts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.credit_limit"), F.col("c.signup_date"))
)

enriched_carts_2 = (
    joined
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.*",
        F.col("d.segment"),
        F.col("d.signup_date").alias("customer_signup_date"),
        F.col("d.credit_limit").alias("customer_credit_limit"),
    )
)

remaining_keys = (
    enriched_carts_2
    .select("customer_id")
    .intersect(
        df_carts
        .filter(F.size("coupon_codes") >= 2)
        .select("customer_id")
    )
)

reconciled = (
    enriched_carts_2
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "element",
        "city",
        "credit_limit",
        "customer_credit_limit",
        F.round((F.col("customer_credit_limit") / F.sum("customer_credit_limit").over(w)), 4).alias("customer_credit_limit_share"),
    )
)

result = (
    scored
    .groupBy("city")
    .pivot("element", ["WELCOME5", "BUNDLE", "SPRING10"])
    .agg(
        F.avg("credit_limit").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
