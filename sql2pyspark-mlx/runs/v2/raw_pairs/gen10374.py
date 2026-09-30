from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .groupBy(
        F.quarter(F.col("s.opened_date")).alias("opened_quarter"),
        F.col("s.region"),
    )
    .agg(
        (F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"),
        F.countDistinct(F.col("ss.revenue")).alias("unique_stores"),
    )
    .filter(F.col("unique_stores") > 4)
)
