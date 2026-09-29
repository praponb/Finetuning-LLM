from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date"), "customer_id")
w2 = Window.partitionBy("country")

lines = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

enriched = (
    lines
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.id", "e.customer_id", F.col("d.name"))
)

remaining_keys = (
    enriched
    .select("customer_id", "id")
    .exceptAll(
        df_carts
        .filter(F.hour("created_at").between(6, 13))
        .select("customer_id", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_carts = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .filter(~F.col("d.city").isin(["Tokyo", "Chiang Mai", "Berlin"]))
    .select(
        "e.id",
        "e.customer_id",
        F.col("d.credit_limit"),
        F.col("d.signup_date"),
        F.col("d.email"),
    )
)

enriched_carts_2 = (
    enriched_carts
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("s.*", F.col("b.email").alias("customer_email"), F.col("b.country"))
)

result = (
    enriched_carts_2
    .withColumn("signup_date_month_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4))
)
