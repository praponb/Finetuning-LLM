from pyspark.sql import functions as F

result = (
    df_employees
    .select("email", "level", F.initcap(F.trim("level")).alias("level_title"))
)
