from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2023-09-01")
    .select("id", "opened_date", "city")
    .unionByName(
        df_stores
        .filter(F.col("city") == "Singapore")
        .select("id", "opened_date", "city")
    )
    .orderBy("city")
)
