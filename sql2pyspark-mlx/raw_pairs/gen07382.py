from pyspark.sql import functions as F

dim_totals = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "segment"], ["city"], []],
        F.col("d.city").alias("city"),
        F.col("d.segment").alias("segment"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.balance")).alias("min_balance"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    dim_totals
    .select("city", "segment", "min_balance")
    .orderBy(F.col("min_balance").desc(), F.col("city").desc())
)
