from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([21, 34, 35, 37]))
    .select("id", "level")
    .unionByName(
        df_employees
        .filter(F.upper(F.trim("full_name")) == "HENRY FORD")
        .select("id", "level")
    )
)
