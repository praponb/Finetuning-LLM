from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L5")
    .select("id", "level", "dept_id")
    .subtract(
        df_employees
        .filter(~F.col("level").isin(["L1", "L4"]))
        .select("id", "level", "dept_id")
    )
)
