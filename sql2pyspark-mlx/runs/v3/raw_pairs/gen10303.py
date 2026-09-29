from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L4")
    .select("level")
    .unionByName(df_employees.filter(F.length("email") > 5).select("level"))
)
