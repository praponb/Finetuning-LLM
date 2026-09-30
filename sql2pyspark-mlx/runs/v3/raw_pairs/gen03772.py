from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") >= 11)
    .select("sqft", "region")
    .unionByName(
        df_stores
        .filter(F.col("opened_date") > "2024-05-02")
        .select("sqft", "region")
    )
)
