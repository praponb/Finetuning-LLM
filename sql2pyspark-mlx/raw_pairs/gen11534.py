from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "signup_date"), "segment").orderBy(F.col("credit_limit").asc_nulls_last())
w2 = Window.partitionBy(F.date_trunc("WEEK", "signup_date"), "customer_segment")
w3 = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy(F.col("credit_limit").desc_nulls_first())
w4 = Window.partitionBy("segment")

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

enriched = (
    lines
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.element",
        F.col("c.segment"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.credit_limit"),
        F.col("p.city"),
        F.col("p.segment").alias("customer_segment"),
    )
)

enriched_carts = (
    joined
    .select(
        "*",
        F.rank().over(w).alias("credit_limit_rank"),
        F.min("credit_limit").over(w2).alias("signup_date_period_min_credit_limit"),
        F.rank().over(w3).alias("credit_limit_rank_2"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4).alias("credit_limit_share"),
    )
)

result = (
    enriched_carts
    .select(
        "customer_id",
        "element",
        "segment",
        "email",
        "signup_date",
        "credit_limit",
        "city",
        "customer_segment",
        "credit_limit_rank",
        F.when(F.col("city").rlike("^(Singapore|Chiang Mai)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|New York)$"), "EXTENDED").otherwise("GROUP_A").alias("city_zone"),
    )
)
