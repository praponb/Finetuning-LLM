from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("sqft") > 5932)

agg = (
    valid_stores
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)

result = (
    agg
    .select("opened_month", "opened_quarter")
    .orderBy(F.col("opened_quarter").desc())
)
