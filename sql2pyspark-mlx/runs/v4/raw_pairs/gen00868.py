from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") <= "2023-11-05") & ((F.col("salary") <= 133000) | (F.col("level") == "L2")))
    .select("id", "hire_date", F.initcap(F.trim("email")).alias("email_title"))
    .orderBy("hire_date", "email_title")
)
