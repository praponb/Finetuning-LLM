from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("city", "country").orderBy("rating", "id")

base = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("Bangkok"))) & F.col("city").isin(["Tokyo", "New York"]))
    .select("*", F.concat_ws("-", "country", "city").alias("country_city_key"))
)

eligible = (
    base
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.size(F.col("x.tags")) >= 1), "left_anti")
)

enriched = (
    eligible
    .withColumn("next_rating", F.lead("rating").over(w_grp))
    .withColumn("prev_rating", F.lag("rating").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

filtered = (
    enriched
    .filter((~F.col("city").eqNullSafe(F.lit("Chiang Mai"))) & F.col("city").isin(["New York", "Tokyo", "Berlin"]))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "country_city_key",
        "prev_rating",
        F.when(F.col("country") == "TH", 1).otherwise(0).alias("is_th"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

result = (
    filtered
    .select("id", "city", "rating", F.col("prev_rating").alias("PREV_RATING"))
    .unpivot(
        ids=["id", "city"],
        values=["rating", "PREV_RATING"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
)
