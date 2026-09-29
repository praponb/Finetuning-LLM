from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "Legal")
        .select("dept_name")
    )
)
