from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-06-28").cast("date"))
    .select(
        "*",
        (F.datediff(F.current_date(), "last_restock") / 365.25).alias("years_since_last_restock"),
        F.round((F.col("quantity") * 100), 2).alias("adjusted_quantity"),
    )
)

result = (
    cleaned
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.region").alias("region"), F.col("s.city").alias("city"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("e.years_since_last_restock")), 2).alias("avg_years_since_last_restock"),
        F.max(F.col("e.quantity")).alias("max_quantity"),
    )
    .orderBy(F.col("avg_years_since_last_restock").desc_nulls_last())
)
