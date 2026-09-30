from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@mail.co.th") & (F.lower(F.trim("email")) == "liam@outlook.com"))
    .select(
        "email",
        "level",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)
