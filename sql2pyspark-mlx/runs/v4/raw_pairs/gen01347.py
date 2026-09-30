from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("level", "full_name")
    .unionByName(
        df_employees
        .filter(F.col("email").like("%@mail.co.th"))
        .select("level", "full_name")
    )
)
