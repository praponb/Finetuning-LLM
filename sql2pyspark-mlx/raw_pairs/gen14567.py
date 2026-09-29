from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("Da%"))
    .select("id", "full_name", F.initcap(F.trim("full_name")).alias("full_name_title"))
)
