from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.revenue").between(1700, 2100) | F.col("s.region").isin(["South", "East", "North"]))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.round((F.col("ss.units") * 1.15), 2).alias("adjusted_units"),
    )
)
