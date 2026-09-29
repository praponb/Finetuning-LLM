from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-06-13").cast("date")) & (F.col("store_id") == 6))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.col("units").cast("double").alias("units_str"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.sqft"), F.col("d.store_name"), F.col("d.region"))
)
