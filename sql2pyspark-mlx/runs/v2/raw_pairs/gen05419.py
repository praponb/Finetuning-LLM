from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L4", "L1", "L2"]))
    .select("id", "level", "is_remote")
)
