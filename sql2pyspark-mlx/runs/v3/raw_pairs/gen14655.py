from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select("email")
    .unionByName(
        df_employees
        .filter(F.col("manager_id").isin([3, 7, 16]))
        .select("email")
    )
    .orderBy("email")
)
