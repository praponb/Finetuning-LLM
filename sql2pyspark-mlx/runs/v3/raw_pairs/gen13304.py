from pyspark.sql import functions as F

src = df_accounts.filter(F.col("status").isin(["OPEN", "FROZEN"]))

opened_year_summary = (
    src
    .groupBy(F.year("opened_date").alias("opened_year"), "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
)

result = (
    opened_year_summary
    .filter(F.col("pct_open") > 50)
    .select("opened_year", "status", "pct_open")
    .orderBy("status", "opened_year")
    .limit(20)
)
