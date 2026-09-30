from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").isNotNull())
    .select("hire_date")
    .unionByName(
        df_products
        .filter(F.col("launch_date").isNotNull())
        .select(F.col("launch_date").alias("hire_date"))
    )
    .orderBy("hire_date")
)
