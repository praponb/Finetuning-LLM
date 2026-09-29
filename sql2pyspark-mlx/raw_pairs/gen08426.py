from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("year_start", F.date_trunc("year", "opened_date"))
    .groupingSets([["status", "year_start"], ["status"]], "status", "year_start")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.min("balance").alias("min_balance"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("min_balance")
)
