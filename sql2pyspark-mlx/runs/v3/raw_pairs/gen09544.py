from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "London")
    .select(
        "id",
        "opened_date",
        "region",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
    )
    .orderBy("region")
)
