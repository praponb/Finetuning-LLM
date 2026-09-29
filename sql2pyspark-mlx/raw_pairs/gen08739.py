from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select("brand")
    .union(
        df_departments
        .filter(F.col("dept_name").isNotNull())
        .select("dept_name")
    )
)
