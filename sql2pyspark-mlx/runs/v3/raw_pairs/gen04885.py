from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.is_active"))
    .groupBy(F.col("c.segment"))
    .agg(
        F.sum(F.when(F.col("o.channel") == "partner", F.col("o.discount")).otherwise(0)).alias("partner_discount"),
    )
    .filter(F.col("partner_discount") >= 26)
    .orderBy(F.col("partner_discount").desc())
)
