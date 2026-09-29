from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select("merchant")
    .subtract(
        df_employees
        .filter(F.col("full_name").isNotNull())
        .select("full_name")
    )
    .orderBy("merchant")
)
