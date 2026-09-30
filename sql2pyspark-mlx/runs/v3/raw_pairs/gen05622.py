from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date", F.lit("store_sale").alias("source"))
    .union(
        df_employees
        .filter(F.col("salary") <= 79000)
        .select("hire_date", F.lit("employee").alias("source"))
    )
)
