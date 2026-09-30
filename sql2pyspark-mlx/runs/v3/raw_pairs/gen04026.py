from pyspark.sql import functions as F

max_end_date = df_subscriptions.filter(F.col("end_date").between("2023-07-24", "2023-12-01")).agg(F.max("end_date")).first()[0]

result = (
    df_subscriptions
    .filter(F.col("end_date") > max_end_date)
    .select("id", "end_date", "start_date")
    .orderBy(F.col("end_date").desc())
)
