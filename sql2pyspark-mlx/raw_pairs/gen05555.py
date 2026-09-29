from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("country")
w_hist = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("rating", "id")

filtered = (
    df_suppliers
    .filter(F.col("rating").between(1.8, 2.7))
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.lower("country").alias("country_lower"),
    )
)

result = (
    filtered
    .select(
        "id",
        "supplier_name",
        "country_lower",
        F.stddev_samp("rating").over(w_ordered).alias("country_stddev_samp_rating"),
        F.sum("rating").over(w_hist).alias("running_rating"),
        F.max("rating").over(w_ordered).alias("country_max_rating"),
        F.lead("rating", 2).over(w_running).alias("next_rating"),
    )
)
