from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .union(
        df_inventory
        .filter(F.col("product_id").isin([1, 2, 3, 7]))
        .select("last_restock")
    )
    .distinct()
    .orderBy("hire_date")
)
