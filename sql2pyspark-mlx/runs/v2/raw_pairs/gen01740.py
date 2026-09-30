from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", F.year("opened_date").alias("opened_year"))
    .agg(F.countDistinct("balance").alias("unique_statuss"))
    .filter(F.col("unique_statuss") < 5)
    .select("status", "opened_year", "unique_statuss")
    .orderBy(F.col("unique_statuss").desc())
)
