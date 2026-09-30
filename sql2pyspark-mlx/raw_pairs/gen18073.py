from pyspark.sql import functions as F

result = (
    df_employees
    .select("level")
    .intersect(
        df_suppliers
        .filter(F.col("supplier_name").like("%Gmb%"))
        .select(F.col("supplier_name").alias("level"))
    )
)
