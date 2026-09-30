from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") < 8)
    .select("full_name")
    .union(df_sessions.select("browser"))
    .distinct()
    .orderBy("full_name")
)
