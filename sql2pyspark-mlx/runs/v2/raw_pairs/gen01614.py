from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.lower(F.trim("email")) == "carol@company.co") & (~F.col("is_remote") | F.col("job_title").isin(["Engineer", "Designer"])))
    .select("is_remote", "full_name", F.substring("email", 1, 2).alias("email_prefix"))
    .orderBy(F.col("full_name").desc(), "email_prefix")
)
