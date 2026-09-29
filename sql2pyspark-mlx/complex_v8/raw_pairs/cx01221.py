from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_by_weight_kg = Window.partitionBy("is_active").orderBy("weight_kg", "id")
w_is_active_by_weight_kg_2 = Window.partitionBy("is_active").orderBy(F.col("weight_kg").desc(), "id")
w_is_active_by_weight_kg_ties = Window.partitionBy("is_active").orderBy(F.col("weight_kg").desc())

base_skus = (
    df_skus
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.least(F.col("unit_cost"), F.col("weight_kg")).alias("least_unit_cost_weight_kg"),
        F.concat_ws(": ", F.col("category"), F.col("sku_code")).alias("category_sku_code_label"),
    )
)

with_trends = (
    base_skus
    .select(
        "id",
        "sku_code",
        "unit_cost",
        "least_unit_cost_weight_kg",
        "category_sku_code_label",
        F.row_number().over(w_is_active_by_weight_kg).alias("weight_kg_seq"),
        F.lag("least_unit_cost_weight_kg", 2).over(w_is_active_by_weight_kg).alias("prev2_least_unit_cost_weight_kg"),
        (F.col("unit_cost") - F.lag("unit_cost").over(w_is_active_by_weight_kg_2)).alias("unit_cost_change"),
        F.rank().over(w_is_active_by_weight_kg_ties).alias("weight_kg_rank"),
    )
)

result = (
    with_trends
    .select("id", "weight_kg_seq", "weight_kg_rank")
    .unpivot(
        ids=["id"],
        values=["weight_kg_seq", "weight_kg_rank"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
)
