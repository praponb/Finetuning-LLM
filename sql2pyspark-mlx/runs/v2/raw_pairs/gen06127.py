from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "manager_id",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)
