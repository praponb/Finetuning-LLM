from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Finance", "Marketing"]) & F.col("location").isin(["London", "New York", "Singapore"]) & F.col("location").isin(["Bangkok", "Berlin"]))
    .select(
        "id",
        "budget",
        F.concat("dept_name", F.lit(" - "), "location").alias("dept_name_location"),
    )
)
