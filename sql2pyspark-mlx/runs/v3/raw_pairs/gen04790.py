from pyspark.sql import functions as F

max_sale_date = df_store_sales.filter(F.col("revenue") >= 1300).agg(F.max("sale_date")).first()[0]

result = (
    df_store_sales
    .filter(F.col("sale_date") == max_sale_date)
    .select("id", "sale_date", "store_id")
    .orderBy("id", F.col("sale_date").desc())
    .limit(20)
)
