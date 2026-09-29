from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull())
    .select("sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("store_id").isin([3, 4, 5, 6]))
        .select("sale_date")
    )
    .orderBy("sale_date")
)
