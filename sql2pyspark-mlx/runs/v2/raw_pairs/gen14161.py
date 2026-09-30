from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .filter(F.col("browser").isNull())
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.row_count") > 7)
    .select(F.col("c.city"), F.col("cs.total_count"), F.col("cs.row_count"))
    .orderBy(F.col("total_count").desc())
)
