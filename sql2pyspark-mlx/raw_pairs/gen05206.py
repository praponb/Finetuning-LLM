from pyspark.sql import functions as F

result = (
    df_customers
    .select("email")
    .unionByName(
        df_employees
        .filter(F.col("level").isin(["L2", "L3", "L1"]))
        .select("email")
    )
    .distinct()
    .orderBy("email")
)
