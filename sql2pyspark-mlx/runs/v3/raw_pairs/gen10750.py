from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-04-01", "2024-10-01"))
    .select(F.col("last_restock").alias("event_date"))
    .unionByName(
        df_employees
        .filter(F.length("email") < 14)
        .select(F.col("hire_date").alias("event_date"))
    )
    .orderBy(F.col("event_date").desc())
)
