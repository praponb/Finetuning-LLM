from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper(F.trim("email")) == "IVY@GMAIL.COM")
    .select("level")
    .unionByName(df_employees.filter(F.col("level").isNotNull()).select("level"))
    .orderBy("level")
)
