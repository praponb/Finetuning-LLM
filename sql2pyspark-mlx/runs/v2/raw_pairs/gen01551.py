from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([1, 9, 12]))
    .select("units", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("units") >= 23)
        .select("units", "store_id")
    )
    .orderBy("store_id")
)
