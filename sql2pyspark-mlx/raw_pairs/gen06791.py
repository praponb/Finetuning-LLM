from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "London")
    .select("id", "store_name")
    .unionByName(
        df_stores
        .filter(F.col("opened_date").between("2023-12-24", "2024-09-28"))
        .select("id", "store_name")
    )
    .orderBy("id")
)
