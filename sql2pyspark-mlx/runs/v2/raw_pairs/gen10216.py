from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L2", "L5", "L3"]))
    .select("id", "full_name")
    .orderBy("full_name")
)
