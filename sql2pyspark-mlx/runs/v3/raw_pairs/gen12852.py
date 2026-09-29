from pyspark.sql import functions as F

result = (
    df_customers
    .select("name")
    .unionByName(
        df_employees
        .filter(F.col("manager_id").isin([27, 37, 38]))
        .select(F.col("full_name").alias("name"))
    )
    .orderBy("name")
)
