from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").isNotNull())
    .select("hire_date")
    .union(
        df_inventory
        .filter(F.col("last_restock").isNotNull())
        .select("last_restock")
    )
    .distinct()
)
