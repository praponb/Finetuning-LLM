from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("store_name").like("Dow%") & (F.col("sqft") < 9960)) | (F.col("opened_date") < "2023-08-04"))
    .select(
        "id",
        "store_name",
        "city",
        F.regexp_replace("store_name", " ", "_").alias("store_name_slug"),
    )
    .orderBy("city", "id")
    .limit(3)
)
