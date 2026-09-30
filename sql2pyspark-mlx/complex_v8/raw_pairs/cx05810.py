from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("category").orderBy(F.col("weight_kg").desc(), F.col("id").desc())

cleaned_skus = (
    df_skus
    .filter(~F.col("is_active"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
    )
)

deduped = (
    cleaned_skus
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .select("id", "unit_cost", "weight_kg")
    .unpivot(
        ids=["id"],
        values=["unit_cost", "weight_kg"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
