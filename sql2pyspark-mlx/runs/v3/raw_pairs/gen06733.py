from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock", F.lit("inventory").alias("source"))
    .unionByName(
        df_employees
        .select(F.col("hire_date").alias("last_restock"), F.lit("employee").alias("source"))
    )
    .distinct()
)
