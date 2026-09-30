from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.lit("employee").alias("source"), "hire_date")
    .union(
        df_store_sales
        .filter(F.col("revenue") >= 3500)
        .select(F.lit("store_sale").alias("source"), "sale_date")
    )
    .orderBy("hire_date")
)
