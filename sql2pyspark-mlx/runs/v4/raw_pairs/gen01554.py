from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("S%") & (F.col("store_name").like("%Cen%") | F.month("opened_date").isin([3, 8, 11])))
    .select(
        "sqft",
        "store_name",
        "city",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
        F.regexp_replace("store_name", " ", "").alias("store_name_slug"),
    )
    .orderBy("store_name_title")
)
