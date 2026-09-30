from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L2", "L3"]) & (F.col("dept_id") == 20))
    .select(
        "id",
        "salary",
        "email",
        "level",
        F.concat("email", F.lit(" "), "full_name").alias("email_full_name"),
    )
    .orderBy("level")
)
