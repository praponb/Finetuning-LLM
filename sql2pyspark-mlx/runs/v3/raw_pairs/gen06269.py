from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .unionByName(
        df_employees
        .filter(F.col("hire_date") >= "2023-09-01")
        .select(F.col("hire_date").alias("launch_date"))
    )
)
