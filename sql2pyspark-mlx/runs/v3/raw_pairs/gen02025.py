from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.quarter(F.col("s.opened_date")).alias("opened_quarter"),
        F.when(F.col("s.city").isin(["Tokyo", "London"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(F.count(F.col("ss.units")).alias("units_count"))
    .filter(F.col("units_count") >= 2)
    .orderBy("units_count")
)
