from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.email").like("%@GMAIL.COM") | (F.col("s.monthly_fee") > 160))
    .select(
        F.col("s.id"),
        F.col("s.start_date"),
        F.col("s.customer_id"),
        F.col("c.age"),
        F.concat(F.col("c.city"), F.lit(", "), F.col("c.segment")).alias("city_segment"),
    )
)
