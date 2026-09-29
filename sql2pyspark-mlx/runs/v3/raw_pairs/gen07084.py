from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~((F.length("email") > 10) & F.lower("email").like("%ack%") & F.col("level").isin(["L3", "L4", "L1"])))
    .select("id", "is_remote", "full_name", F.floor("salary").alias("salary_whole"))
)
