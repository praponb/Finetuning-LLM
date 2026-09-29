from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") >= 9)
    .select("id", "city", "opened_date")
    .unionByName(
        df_stores
        .filter(F.col("sqft") <= 7670)
        .select("id", "city", "opened_date")
    )
    .orderBy("opened_date")
)
