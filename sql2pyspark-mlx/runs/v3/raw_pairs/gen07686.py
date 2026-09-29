from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper(F.trim("email")) == "JACK@STARTUP.IO")
    .select("id", "full_name", "email")
    .unionByName(
        df_employees
        .filter(~F.col("is_remote"))
        .select("id", "full_name", "email")
    )
    .distinct()
    .orderBy("id")
)
