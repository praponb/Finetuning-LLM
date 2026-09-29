from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w_running = Window.partitionBy("city")
w3 = Window.orderBy("rating", "id")

base = (
    df_suppliers
    .filter(~F.col("city").isin(["Chiang Mai", "Tokyo"]))
    .select("*", F.length("country").alias("country_length"))
)

result = (
    base
    .filter(F.col("country_length") != 46)
    .select(
        "id",
        "supplier_name",
        "city",
        "country_length",
        F.sum("country_length").over(w).alias("country_sum_country_length"),
        F.max("country_length").over(w_running).alias("city_max_country_length"),
        F.lead("rating", 1).over(w3).alias("next_rating"),
        F.stddev_samp("country_length").over(w_running).alias("city_stddev_samp_country_length"),
    )
)
