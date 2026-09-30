from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower(F.trim("full_name")) == "jack taylor")
    .select("dept_id")
    .unionByName(
        df_employees
        .filter(~F.col("level").isin(["L2", "L4", "L1"]))
        .select("dept_id")
    )
)
