from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(3500, 3900))
    .select("id", "sale_date", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.col("store_id") == 4)
        .select("id", "sale_date", "store_id")
    )
    .distinct()
    .orderBy("id")
)
