from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L2")
    .select("full_name")
    .unionByName(
        df_employees
        .filter(F.upper("full_name").like("%AVI%"))
        .select("full_name")
    )
)
